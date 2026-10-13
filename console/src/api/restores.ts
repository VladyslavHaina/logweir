// The Restores page group's reads and writes (PROD-18.1 R5), over the typed client.
// Lists re-read while a run in them moves; one restore's page follows its operation
// while it is not final. Every write carries the Idempotency-Key the plan's minted
// name gives it, so a second click, a lost answer or a reload never makes a second object.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, api, createHeaders, unwrap } from "./client";
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

const LIVE_MS = 1500;
const PAGE = 200;
const PAGE_BUDGET = 25;

export const restoreKeys = {
  list: (ns: string) => ["restores", ns] as const,
  item: (ns: string, name: string) => ["restores", ns, name] as const,
  operation: (ns: string, name: string) => ["operations", ns, "restore", name] as const,
  approvalRequest: (ns: string, name: string) => ["restores", ns, name, "approval-request"] as const,
  policy: (ns: string) => ["approval-policy", ns] as const,
  preflight: (ns: string, id: string) => ["preflights", ns, id] as const,
};

/** A refusal that keeps the problem's field errors. */
export class RestoreProblem extends ApiError {
  readonly fieldErrors: S["FieldError"][];
  constructor(status: number, problem: Partial<S["Problem"]> | undefined) {
    super(status, problem);
    this.fieldErrors = Array.isArray(problem?.errors) ? problem.errors : [];
  }
}

/** A write that reached no answer, or a gateway that gave up: it may or may not have landed. */
export function outcomeUnknown(error: unknown): boolean {
  if (!(error instanceof ApiError)) return true;
  return error.status === 502 || error.status === 503 || error.status === 504 || error.code === "upstream_timeout";
}

function unwrapWrite<T>(result: { data?: T; error?: unknown; response: Response }): T {
  if (result.data !== undefined && result.response.ok) return result.data;
  throw new RestoreProblem(result.response.status, result.error as Partial<S["Problem"]> | undefined);
}

/** Every page of a list, following nextCursor. A list past the budget is an error, never a prefix shown as the whole. */
async function listAll<T>(what: string, read: (cursor: string | undefined) => Promise<{ items: T[]; page: { nextCursor?: string | null } }>): Promise<T[]> {
  const items: T[] = [];
  let cursor: string | undefined;
  for (let pages = 0; pages < PAGE_BUDGET; pages += 1) {
    const page = await read(cursor);
    items.push(...page.items);
    if (!page.page.nextCursor) return items;
    cursor = page.page.nextCursor;
  }
  throw new ApiError(0, { code: "list_too_large" as S["Problem"]["code"], detail: `This namespace holds more than ${items.length} ${what}, more than the console reads in one list.` });
}

/** What this session may do in a namespace. */
export function useCapabilities(ns: string): Capabilities | undefined {
  const s = useSessionDoc();
  return grantOf(s, ns)?.capabilities ?? s.capabilities;
}

// ------------------------------------------------------------------- reads

const moving = (items: { operation: { terminal: boolean } }[] | undefined) => (items?.some((i) => !i.operation.terminal) ? LIVE_MS : false);
const notFound = (error: unknown) => error instanceof ApiError && (error.status === 404 || error.status === 403);

export function useRestoreList(ns: string) {
  return useQuery({
    queryKey: restoreKeys.list(ns),
    queryFn: ({ signal }) =>
      listAll("restores", async (cursor) =>
        unwrap(await api.GET("/api/v1/namespaces/{ns}/restores", { params: { path: { ns }, query: { limit: PAGE, ...(cursor ? { cursor } : {}) } }, signal })),
      ),
    refetchInterval: (q) => moving(q.state.data),
  });
}

export function useRestoreItem(ns: string, name: string) {
  return useQuery({
    queryKey: restoreKeys.item(ns, name),
    queryFn: async ({ signal }) => unwrap(await api.GET("/api/v1/namespaces/{ns}/restores/{name}", { params: { path: { ns, name } }, signal })).item,
    refetchInterval: (q) => (q.state.data && !q.state.data.operation.terminal ? LIVE_MS : false),
    retry: (count, error) => !notFound(error) && count < 2,
  });
}

/** THE LIVE WATCH: the run's operation, re-read every second while it is not final.
 *  (R1's operation stream feeds the same cache key when the page is served live.) */
export function useRestoreOperation(ns: string, name: string, enabled = true) {
  return useQuery({
    queryKey: restoreKeys.operation(ns, name),
    enabled,
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/operations/{kind}/{name}", { params: { path: { ns, kind: "restore", name } }, signal })).item,
    refetchInterval: (q) => (q.state.data && !q.state.data.terminal ? 1000 : false),
    retry: (count, error) => !notFound(error) && count < 2,
  });
}

/** The two-person request a restore waits on, when there is one (404 when none was made). */
export function useApprovalRequest(ns: string, name: string, enabled: boolean) {
  return useQuery({
    queryKey: restoreKeys.approvalRequest(ns, name),
    enabled,
    queryFn: async ({ signal }) => {
      const r = await api.GET("/api/v1/namespaces/{ns}/restores/{name}/approval-request", { params: { path: { ns, name } }, signal });
      if (r.response.status === 404) return null;
      return unwrap(r).item;
    },
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
        return unwrap(await api.GET("/api/v1/namespaces/{ns}/approval-policy", { params: { path: { ns, name: "" } }, signal })).item;
      } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError") throw error;
        return null;
      }
    },
    // Read afresh on every visit: it decides where a submission goes.
    staleTime: 0,
  });
}

export function useBackupList(ns: string) {
  return useQuery({
    queryKey: ["backups", ns, "all"],
    queryFn: ({ signal }) =>
      listAll("backups", async (cursor) =>
        unwrap(await api.GET("/api/v1/namespaces/{ns}/backups", { params: { path: { ns }, query: { limit: PAGE, ...(cursor ? { cursor } : {}) } }, signal })),
      ),
  });
}

export function useConnectionList(ns: string) {
  return useQuery({
    queryKey: ["connections", ns, "all"],
    queryFn: ({ signal }) =>
      listAll("connections", async (cursor) =>
        unwrap(await api.GET("/api/v1/namespaces/{ns}/connections", { params: { path: { ns }, query: { limit: PAGE, ...(cursor ? { cursor } : {}) } }, signal })),
      ),
  });
}

export function useDestinationList(ns: string, enabled: boolean) {
  return useQuery({
    queryKey: ["destinations", ns, "all"],
    enabled,
    queryFn: ({ signal }) =>
      listAll("destinations", async (cursor) =>
        unwrap(await api.GET("/api/v1/namespaces/{ns}/destinations", { params: { path: { ns }, query: { limit: PAGE, ...(cursor ? { cursor } : {}) } }, signal })),
      ),
  });
}

export async function readDestination(ns: string, name: string, signal?: AbortSignal): Promise<Destination | null> {
  const r = await api.GET("/api/v1/namespaces/{ns}/destinations/{name}", { params: { path: { ns, name } }, signal });
  if (r.response.status === 404) return null;
  return unwrap(r).item;
}

export function useDestination(ns: string, name: string) {
  return useQuery({
    queryKey: ["destinations", ns, "item", name],
    enabled: name.length > 0,
    queryFn: ({ signal }) => readDestination(ns, name, signal),
  });
}

export async function listCatalogs(ns: string, signal?: AbortSignal): Promise<CatalogView[]> {
  return listAll("catalogs", async (cursor) =>
    unwrap(await api.GET("/api/v1/namespaces/{ns}/catalogs", { params: { path: { ns }, query: { limit: PAGE, ...(cursor ? { cursor } : {}) } }, signal })),
  );
}

export async function readCatalog(ns: string, name: string, signal?: AbortSignal): Promise<CatalogView> {
  return unwrap(await api.GET("/api/v1/namespaces/{ns}/catalogs/{name}", { params: { path: { ns, name } }, signal })).item;
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
    const page = unwrap(
      await api.GET("/api/v1/namespaces/{ns}/catalogs/{name}/points", {
        params: { path: { ns, name: catalog }, query: { limit: PAGE, ...(cursor ? { cursor } : {}), ...(selectable ? { selectable: "true" as const } : {}) } },
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
        const page = unwrap(
          await api.GET("/api/v1/namespaces/{ns}/catalogs/{name}/points", {
            params: { path: { ns, name: catalog.name }, query: { limit: PAGE, selectable: "true", ...(cursor ? { cursor } : {}) } },
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
  const op = unwrap(await api.GET("/api/v1/namespaces/{ns}/operations/{kind}/{name}", { params: { path: { ns, kind: "backup", name } }, signal })).item;
  return op.evidence.payloadSha256 ?? null;
}

/** The target's newest topic discoveries (`?latest=true`), for its broker count. */
export async function readLatestDiscoveries(ns: string, connection: string, signal?: AbortSignal) {
  const r = await api.GET("/api/v1/namespaces/{ns}/connections/{name}/topic-discoveries", {
    params: { path: { ns, name: connection }, query: { latest: true } },
    signal,
  });
  return unwrap(r) as unknown as { latestAttempt?: TopicDiscovery | null; lastSuccessful?: TopicDiscovery | null };
}

// ------------------------------------------------------------------- writes

/** Start a restore Preflight for exactly the prepared bytes. One deliberate click is one
 *  observation of mutable state, so each click mints its own key. */
export async function startPreflight(ns: string, body: S["CreatePreflightRequest"], key: string): Promise<Preflight> {
  return unwrapWrite(await api.POST("/api/v1/namespaces/{ns}/preflights", { params: { path: { ns }, header: createHeaders(key) }, body })).item;
}

/** Read a preflight against the hash on screen: the server recomputes `applicable` and `stale`. */
export async function readPreflight(ns: string, id: string, planHash: string, signal?: AbortSignal): Promise<Preflight> {
  return unwrap(await api.GET("/api/v1/namespaces/{ns}/preflights/{id}", { params: { path: { ns, id }, query: planHash ? { planHash } : {} }, signal })).item;
}

export async function cancelPreflight(ns: string, id: string): Promise<void> {
  unwrapWrite(await api.POST("/api/v1/namespaces/{ns}/preflights/{id}:cancel", { params: { path: { ns, id }, header: { Origin: window.location.origin } } }));
}

export function useCreateRestore(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ body, key }: { body: S["CreateRestoreRequest"]; key: string }) =>
      unwrapWrite(await api.POST("/api/v1/namespaces/{ns}/restores", { params: { path: { ns }, header: createHeaders(key) }, body })),
    onSuccess: () => client.invalidateQueries({ queryKey: restoreKeys.list(ns) }),
  });
}
