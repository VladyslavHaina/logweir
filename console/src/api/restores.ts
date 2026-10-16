// The Restores page group's reads and writes (PROD-18.1 R5), over the typed client.
// Lists re-read while a run in them moves; one restore's page follows its operation
// while it is not final. Every write carries the Idempotency-Key the plan's minted
// name gives it, so a second click, a lost answer or a reload never makes a second object.
import { useEffect, useState } from "react";
import { useMutation, useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, LIST_PAGE_BUDGET, LIST_PAGE_SIZE, api, call, createHeaders, listAll } from "./client";
import { keys, useBackups, useConnections, useDestinations, useOperation, useRestore, useRestores, verdictPending } from "./queries";

export { verdictPending };
import { grantOf, useCan, useSessionDoc } from "./session";
import type { Clone } from "./switchovers";
import type { Client } from "openapi-fetch";
import type { components, paths } from "./schema";

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
  list: (ws: string) => keys.list("restores", ws),
  item: (ws: string, name: string) => keys.item("restores", ws, name),
  operation: (ws: string, name: string) => keys.operation(ws, "restore", name),
  /** Null when no request was made: kept apart from the Approvals page's read, which throws. */
  approvalRequest: (ws: string, name: string) => ["restore-approval-requests", ws, name] as const,
  /** The fail-safe reading (null when unreadable), kept apart from the Approvals page's. */
  policy: (ws: string) => ["approval-policy", ws, "fail-safe"] as const,
  preflight: (ws: string, id: string) => ["preflights", ws, id] as const,
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

/** What this session may do in a workspace. */
export function useCapabilities(ws: string): Capabilities | undefined {
  const s = useSessionDoc();
  return grantOf(s, ws)?.capabilities ?? s.capabilities;
}

// ------------------------------------------------------------------- reads

/** The workspace's restores, newest first: R1's list, one cache with Approvals and the Overview. */
export const useRestoreList = useRestores;
/** One restore: R1's read. */
export const useRestoreItem = useRestore;

/** THE LIVE WATCH: the run's operation (R1's read), re-read every second while it is not
 *  final unless the run's event stream (stream.ts) is carrying it. */
/** How far back a scratch restore may still be a live clone: the longest lifetime (30 days) and a day for its run. */
const CLONE_HORIZON_MS = 31 * 86_400_000;
/** The most operation views the Restores list reads for clones. */
export const CLONE_READS = 50;

/**
 * DUI-23: a list row carries no clone; its operation view does (`status.clone`). Each
 * recent scratch restore's view is read, at most CLONE_READS, under the same key its page
 * uses, so the two share one cache entry. A view that cannot be read leaves its row as it was.
 */
export function useClones(ws: string, restores: Restore[] | undefined): Map<string, Clone> {
  const readable = useCan(ws, "operationsRead");
  const since = Date.now() - CLONE_HORIZON_MS;
  const wanted = readable
    ? (restores ?? []).filter((r) => r.target.mode === "scratch" && (Date.parse(r.createdAt ?? "") || 0) >= since).slice(0, CLONE_READS)
    : [];
  const results = useQueries({
    queries: wanted.map((r) => ({
      queryKey: keys.operation(ws, "restore", r.name),
      queryFn: async ({ signal }: { signal: AbortSignal }) =>
        (await call(() => api.GET("/api/v1/workspaces/{ws}/operations/{kind}/{name}", { params: { path: { ws, kind: "restore", name: r.name } }, signal }))).item,
      staleTime: 30_000,
      retry: false,
    })),
  });
  const out = new Map<string, Clone>();
  results.forEach((q, i) => {
    if (q.data?.clone) out.set(wanted[i]!.name, q.data.clone);
  });
  return out;
}

export function useRestoreOperation(ws: string, name: string, streaming = false, follow = false) {
  return useOperation(ws, "restore", name, streaming, follow);
}

/** How long a page follows a terminal restore whose verdict is still pending (DUI-38). */
export const VERDICT_FOLLOW_MS = 120_000;

/**
 * DUI-38: whether the page still follows a terminal restore whose verdict is pending: from
 * the moment it saw it pending, for VERDICT_FOLLOW_MS, then it stops (`stopped`) until
 * `again`. A verdict that lands ends it at once.
 */
export function useVerdictFollow(pending: boolean): { following: boolean; stopped: boolean; again: () => void } {
  const [round, setRound] = useState(0);
  const [stopped, setStopped] = useState(false);
  useEffect(() => {
    if (!pending) return undefined;
    const timer = setTimeout(() => setStopped(true), VERDICT_FOLLOW_MS);
    return () => clearTimeout(timer);
  }, [pending, round]);
  return {
    following: pending && !stopped,
    stopped: pending && stopped,
    again: () => {
      setStopped(false);
      setRound((n) => n + 1);
    },
  };
}

/** The two-person request a restore waits on, when there is one (404 when none was made). */
export function useApprovalRequest(ws: string, name: string, enabled: boolean) {
  return useQuery({
    queryKey: restoreKeys.approvalRequest(ws, name),
    enabled,
    queryFn: ({ signal }) =>
      orNull(async () => (await call(() => api.GET("/api/v1/workspaces/{ws}/restores/{name}/approval-request", { params: { path: { ws, name } }, signal }))).item),
    refetchInterval: 5000,
  });
}

/** The workspace's EFFECTIVE approval policy. A read that fails is null: the page then
 *  shows the out-of-band (strict) instructions, the fail-safe reading. */
export function useApprovalPolicy(ws: string) {
  return useQuery({
    queryKey: restoreKeys.policy(ws),
    queryFn: async ({ signal }): Promise<ApprovalPolicy | null> => {
      try {
        // The published operation names a stray `name` path parameter this route does not have;
        // openapi-fetch drops what the path does not use.
        return (await call(() => api.GET("/api/v1/workspaces/{ws}/approval-policy", { params: { path: { ws, name: "" } }, signal }))).item;
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

export async function readDestination(ws: string, name: string, signal?: AbortSignal): Promise<Destination | null> {
  return orNull(async () => (await call(() => api.GET("/api/v1/workspaces/{ws}/destinations/{name}", { params: { path: { ws, name } }, signal }))).item);
}

/** One destination, null when it does not exist (kept apart from R2's read, which throws on 404). */
export function useDestination(ws: string, name: string) {
  return useQuery({
    queryKey: ["destinations-or-null", ws, name],
    enabled: name.length > 0,
    queryFn: ({ signal }) => readDestination(ws, name, signal),
  });
}

export async function listCatalogs(ws: string, signal?: AbortSignal): Promise<CatalogView[]> {
  return listAll("catalogs", (cursor) =>
    call(() => api.GET("/api/v1/workspaces/{ws}/catalogs", { params: { path: { ws }, query: { limit: PAGE, cursor } }, signal })),
  );
}

export async function readCatalog(ws: string, name: string, signal?: AbortSignal): Promise<CatalogView> {
  return (await call(() => api.GET("/api/v1/workspaces/{ws}/catalogs/{name}", { params: { path: { ws, name } }, signal }))).item;
}

export interface PointSearch {
  entry: PointView | null;
  viewExpired: boolean;
  backupVerdictsIncomplete: string | null;
  incomplete: boolean;
  budgetExhausted: boolean;
}

/** Find one point in a catalog's view, page by page (the view holds at most 5000). */
export async function findCatalogPoint(ws: string, catalog: string, match: (p: PointView) => boolean, signal?: AbortSignal, selectable = false): Promise<PointSearch> {
  const out: PointSearch = { entry: null, viewExpired: false, backupVerdictsIncomplete: null, incomplete: false, budgetExhausted: false };
  let cursor: string | undefined;
  for (let read = 0; read < PAGE_BUDGET; read += 1) {
    const at = cursor;
    const page = await call(() =>
      api.GET("/api/v1/workspaces/{ws}/catalogs/{name}/points", {
        params: { path: { ws, name: catalog }, query: { limit: PAGE, ...(at ? { cursor: at } : {}), ...(selectable ? { selectable: "true" as const } : {}) } },
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
export async function readCatalogOffers(ws: string, signal?: AbortSignal) {
  const offers: { catalog: CatalogView; entry: PointView; page: { viewExpired: boolean; backupVerdictsIncomplete: string | null } }[] = [];
  const notes: string[] = [];
  let catalogs: CatalogView[];
  try {
    catalogs = await listCatalogs(ws, signal);
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
          api.GET("/api/v1/workspaces/{ws}/catalogs/{name}/points", {
            params: { path: { ws, name: catalog.name }, query: { limit: PAGE, selectable: "true", ...(at ? { cursor: at } : {}) } },
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
export async function readBackupReceiptDigest(ws: string, name: string, signal?: AbortSignal): Promise<string | null> {
  const op = (await call(() => api.GET("/api/v1/workspaces/{ws}/operations/{kind}/{name}", { params: { path: { ws, kind: "backup", name } }, signal }))).item;
  return op.evidence.payloadSha256 ?? null;
}

/** The target's newest topic discoveries (`?latest=true`), for its broker count. */
export async function readLatestDiscoveries(ws: string, connection: string, signal?: AbortSignal) {
  const r = await call(() =>
    api.GET("/api/v1/workspaces/{ws}/connections/{name}/topic-discoveries", {
      params: { path: { ws, name: connection }, query: { latest: true } },
      signal,
    }),
  );
  return r as unknown as { latestAttempt?: TopicDiscovery | null; lastSuccessful?: TopicDiscovery | null };
}

// ------------------------------------------------------------------- writes

/** Start a restore Preflight for exactly the prepared bytes. One deliberate click is one
 *  observation of mutable state, so each click mints its own key. */
export async function startPreflight(ws: string, body: S["CreatePreflightRequest"], key: string): Promise<Preflight> {
  return (await call(() => api.POST("/api/v1/workspaces/{ws}/preflights", { params: { path: { ws }, header: createHeaders(key) }, body }))).item;
}

/** Read a preflight against the hash on screen: the server recomputes `applicable` and `stale`. */
export async function readPreflight(ws: string, id: string, planHash: string, signal?: AbortSignal): Promise<Preflight> {
  return (await call(() => api.GET("/api/v1/workspaces/{ws}/preflights/{id}", { params: { path: { ws, id }, query: planHash ? { planHash } : {} }, signal }))).item;
}

export async function cancelPreflight(ws: string, id: string): Promise<void> {
  await call(() => api.POST("/api/v1/workspaces/{ws}/preflights/{id}:cancel", { params: { path: { ws, id }, header: { Origin: window.location.origin } } }));
}

export function useCreateRestore(ws: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ body, key }: { body: S["CreateRestoreRequest"]; key: string }) =>
      call(() => api.POST("/api/v1/workspaces/{ws}/restores", { params: { path: { ws }, header: createHeaders(key) }, body })),
    onSuccess: () => client.invalidateQueries({ queryKey: restoreKeys.list(ws) }),
  });
}

/** PROD-07.2: Cancel, or Clean up an interrupted restore's partial topics. The answer is the
 *  Restore as the request left it; the page then follows it as it moves. */
export function useRestoreAction(ws: string, name: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: (action: "cancel" | "cleanup") => restoreAction(ws, name, action),
    onSuccess: (item) => {
      client.setQueryData(restoreKeys.item(ws, name), item);
      void client.invalidateQueries({ queryKey: restoreKeys.list(ws) });
      void client.invalidateQueries({ queryKey: restoreKeys.operation(ws, name) });
    },
  });
}

// PROD-07.2's two routes, typed here until the OpenAPI document carries them (see od23.ts).

type Action = {
  parameters: { query?: never; header: { Origin: string }; path: { ws: string; name: string }; cookie?: never };
  requestBody?: never;
  responses: {
    200: { headers: { [name: string]: unknown }; content: { "application/json": S["RestoreResponse"] } };
    default: { headers: { [name: string]: unknown }; content: { "application/problem+json": S["Problem"] } };
  };
};
type ActionPath = {
  parameters: { query?: never; header?: never; path?: never; cookie?: never };
  get?: never;
  put?: never;
  post: Action;
  delete?: never;
  options?: never;
  head?: never;
  patch?: never;
  trace?: never;
};
type Od23Paths = {
  "/api/v1/workspaces/{ws}/restores/{name}/cancel": ActionPath;
  "/api/v1/workspaces/{ws}/restores/{name}/cleanup": ActionPath;
};

const actions = api as unknown as Client<paths & Od23Paths>;

/** Cancel or Clean up: an annotation on the Restore, so a repeated click writes nothing (no Idempotency-Key). */
export async function restoreAction(ws: string, name: string, action: "cancel" | "cleanup"): Promise<S["Restore"]> {
  const path = action === "cancel" ? "/api/v1/workspaces/{ws}/restores/{name}/cancel" : "/api/v1/workspaces/{ws}/restores/{name}/cleanup";
  return (await call(() => actions.POST(path, { params: { path: { ws, name }, header: { Origin: window.location.origin } } }))).item;
}
